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

package org.apache.hop.pipeline.transforms.rest;

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
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
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

import java.util.List;
import java.util.*;

public class RestDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = RestMeta.class; // For Translator

  private ComboVar wApplicationType;

  private Label wlMethod;
  private ComboVar wMethod;

  private Label wlUrl;
  private TextVar wUrl;

  private TextVar wResult;

  private TextVar wResultCode;

  private TableView wFields;

  private Button wUrlInField;

  private Label wlUrlField;
  private ComboVar wUrlField;

  private Button wMethodInField;

  private Button wPreemptive;

  private Label wlMethodField;
  private ComboVar wMethodField;

  private final RestMeta input;

  private final Map<String, Integer> inputFields;

  private Label wlBody;
  private ComboVar wBody;

  private ColumnInfo[] colinf;
  private ColumnInfo[] colinfoparams;

  private String[] fieldNames;

  private TextVar wHttpLogin;

  private TextVar wHttpPassword;

  private TextVar wProxyHost;

  private TextVar wProxyPort;

  private Label wlParameters;
  private Label wlMatrixParameters;
  private TableView wParameters;
  private TableView wMatrixParameters;

  private TextVar wResponseTime;
  private TextVar wResponseHeader;

  private TextVar wTrustStorePassword;

  private TextVar wTrustStoreFile;

  private Button wbTrustStoreFile;

  private Button wIgnoreSsl;

  private boolean gotPreviousFields = false;

  private Button wMatrixGet;

  public RestDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (RestMeta) in;
    inputFields = new HashMap<>();
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "RestDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    setupButtons(margin);

    setupTransformName(lsMod, middle, margin);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, PropsUi.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////
    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText(BaseMessages.getString(PKG, "RestDialog.GeneralTab.Title"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wGeneralComp.setLayout(fileLayout);

    // ////////////////////////
    // START Settings GROUP

    Group gSettings = setupSettingGroup(wGeneralComp);

    setupUrlLine(lsMod, middle, margin, wGeneralComp, gSettings);
    setupUrlInFieldLine(middle, margin, gSettings);
    setupUrlFieldNameLine(lsMod, middle, margin, gSettings);
    setupHttpMethodLine(lsMod, middle, margin, gSettings);
    setupMethodInFieldLine(middle, margin, gSettings);
    setupMethodNameLine(lsMod, middle, margin, gSettings);
    setupBodyLine(lsMod, middle, margin, gSettings);
    setupAppTypeLine(lsMod, middle, margin, gSettings);

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

    setupResultLine(lsMod, middle, margin, gSettings, gOutputFields);
    setupStatusCodeLine(lsMod, middle, margin, gOutputFields);
    setupResponseTimeLine(lsMod, middle, margin, gOutputFields);
    setupResponseHeaderLine(lsMod, middle, margin, gSettings, gOutputFields);

    // END Output Fields GROUP
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

    // Auth tab...
    //
    CTabItem wAuthTab = new CTabItem(wTabFolder, SWT.NONE);
    wAuthTab.setText(BaseMessages.getString(PKG, "RestDialog.Auth.Title"));

    FormLayout alayout = new FormLayout();
    alayout.marginWidth = Const.FORM_MARGIN;
    alayout.marginHeight = Const.FORM_MARGIN;

    Composite wAuthComp = new Composite(wTabFolder, SWT.NONE);
    wAuthComp.setLayout(alayout);
    props.setLook(wAuthComp);

    // ////////////////////////
    // START HTTP AUTH GROUP

    Group gHttpAuth = setupAuthGroup(wAuthComp);

    setupLoginLine(lsMod, middle, margin, gHttpAuth);
    setupPasswordLine(lsMod, middle, margin, gHttpAuth);
    setupPreemptiveLine(middle, margin, gHttpAuth);

    FormData fdHttpAuth = new FormData();
    fdHttpAuth.left = new FormAttachment(0, 0);
    fdHttpAuth.right = new FormAttachment(100, 0);
    fdHttpAuth.top = new FormAttachment(gOutputFields, margin);
    gHttpAuth.setLayoutData(fdHttpAuth);

    // END HTTP AUTH GROUP
    // ////////////////////////

    // ////////////////////////
    // START PROXY GROUP

    Group gProxy = setupProxyGroup(wAuthComp);

    setupProxyHostLine(lsMod, middle, margin, gProxy);
    setupProxyPortLine(lsMod, middle, margin, gProxy);

    FormData fdProxy = new FormData();
    fdProxy.left = new FormAttachment(0, 0);
    fdProxy.right = new FormAttachment(100, 0);
    fdProxy.top = new FormAttachment(gHttpAuth, margin);
    gProxy.setLayoutData(fdProxy);

    // END HTTP AUTH GROUP
    // ////////////////////////

    FormData fdAuthComp = new FormData();
    fdAuthComp.left = new FormAttachment(0, 0);
    fdAuthComp.top = new FormAttachment(wTransformName, margin);
    fdAuthComp.right = new FormAttachment(100, 0);
    fdAuthComp.bottom = new FormAttachment(100, 0);
    wAuthComp.setLayoutData(fdAuthComp);

    wAuthComp.layout();
    wAuthTab.setControl(wAuthComp);
    // ////// END of Auth Tab

    // SSL tab...
    //
    CTabItem wSSLTab = new CTabItem(wTabFolder, SWT.NONE);
    wSSLTab.setText(BaseMessages.getString(PKG, "RestDialog.SSL.Title"));

    FormLayout ssll = new FormLayout();
    ssll.marginWidth = Const.FORM_MARGIN;
    ssll.marginHeight = Const.FORM_MARGIN;

    Composite wSSLComp = new Composite(wTabFolder, SWT.NONE);
    wSSLComp.setLayout(ssll);
    props.setLook(wSSLComp);

    // ////////////////////////
    // START SSLTrustStore GROUP

    Group gSSLTrustStore = setupTrustoreGroup(wSSLComp);

    Button wbTrustStoreFile = setupTrustoreFileLine(lsMod, middle, margin, gSSLTrustStore);
    setupTrustorePwdLine(lsMod, middle, margin, gSSLTrustStore, wbTrustStoreFile);
    setupIgnoreSslLine(middle, margin, gSSLTrustStore);

    FormData fdSSLTrustStore = new FormData();
    fdSSLTrustStore.left = new FormAttachment(0, 0);
    fdSSLTrustStore.right = new FormAttachment(100, 0);
    fdSSLTrustStore.top = new FormAttachment(gHttpAuth, margin);
    gSSLTrustStore.setLayoutData(fdSSLTrustStore);

    // END HTTP AUTH GROUP
    // ////////////////////////

    FormData fdSSLComp = new FormData();
    fdSSLComp.left = new FormAttachment(0, 0);
    fdSSLComp.top = new FormAttachment(wTransformName, margin);
    fdSSLComp.right = new FormAttachment(100, 0);
    fdSSLComp.bottom = new FormAttachment(100, 0);
    wSSLComp.setLayoutData(fdSSLComp);

    wSSLComp.layout();
    wSSLTab.setControl(wSSLComp);
    // ////// END of SSL Tab

    // Additional tab...
    //
    CTabItem wAdditionalTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdditionalTab.setText(BaseMessages.getString(PKG, "RestDialog.Headers.Title"));

    FormLayout addLayout = new FormLayout();
    addLayout.marginWidth = Const.FORM_MARGIN;
    addLayout.marginHeight = Const.FORM_MARGIN;

    Composite wAdditionalComp = new Composite(wTabFolder, SWT.NONE);
    wAdditionalComp.setLayout(addLayout);
    props.setLook(wAdditionalComp);

    setupHeaderTabContent(lsMod, margin, wAdditionalComp);

    FormData fdAdditionalComp = new FormData();
    fdAdditionalComp.left = new FormAttachment(0, 0);
    fdAdditionalComp.top = new FormAttachment(wTransformName, margin);
    fdAdditionalComp.right = new FormAttachment(100, -margin);
    fdAdditionalComp.bottom = new FormAttachment(100, 0);
    wAdditionalComp.setLayoutData(fdAdditionalComp);

    wAdditionalComp.layout();
    wAdditionalTab.setControl(wAdditionalComp);
    // ////// END of Additional Tab

    // Query Parameters tab...
    //
    CTabItem wParametersTab = new CTabItem(wTabFolder, SWT.NONE);
    wParametersTab.setText(BaseMessages.getString(PKG, "RestDialog.Parameters.Title"));

    FormLayout playout = new FormLayout();
    playout.marginWidth = Const.FORM_MARGIN;
    playout.marginHeight = Const.FORM_MARGIN;

    Composite wParametersComp = new Composite(wTabFolder, SWT.NONE);
    wParametersComp.setLayout(playout);
    props.setLook(wParametersComp);

    setupParameterTabContent(lsMod, margin, wParametersComp);

    FormData fdParametersComp = new FormData();
    fdParametersComp.left = new FormAttachment(0, 0);
    fdParametersComp.top = new FormAttachment(wTransformName, margin);
    fdParametersComp.right = new FormAttachment(100, 0);
    fdParametersComp.bottom = new FormAttachment(100, 0);
    wParametersComp.setLayoutData(fdParametersComp);

    wParametersComp.layout();
    wParametersTab.setControl(wParametersComp);
    // ////// END of Query Parameters Tab

    // Matrix Parameters tab
    CTabItem wMatrixParametersTab = new CTabItem(wTabFolder, SWT.NONE);
    wMatrixParametersTab.setText(BaseMessages.getString(PKG, "RestDialog.MatrixParameters.Title"));

    FormLayout pl = new FormLayout();
    pl.marginWidth = Const.FORM_MARGIN;
    pl.marginHeight = Const.FORM_MARGIN;

    Composite wMatrixParametersComp = new Composite(wTabFolder, SWT.NONE);
    wMatrixParametersComp.setLayout(pl);
    props.setLook(wMatrixParametersComp);

    setupMatrixParamTabContent(lsMod, margin, wMatrixParametersTab, wMatrixParametersComp);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    //
    // Search the fields in the background
    //

    final Runnable runnable =
        new Runnable() {
          @Override
          public void run() {
            TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
            if (transformMeta != null) {
              try {
                IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

                // Remember these fields...
                for (int i = 0; i < row.size(); i++) {
                  inputFields.put(row.getValueMeta(i).getName(), i);
                }

                setComboBoxes();
              } catch (HopException e) {
                log.logError(
                    toString(),
                    BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
              }
            }
          }
        };
    new Thread(runnable).start();

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
    activateUrlInfield();
    activateMethodInfield();
    activateTrustoreFields();
    setMethod();
    input.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void setupMatrixParamTabContent(ModifyListener lsMod, int margin, CTabItem wMatrixParametersTab, Composite wMatrixParametersComp) {
    wlMatrixParameters = new Label(wMatrixParametersComp, SWT.NONE);
    wlMatrixParameters.setText(BaseMessages.getString(PKG, "RestDialog.Parameters.Label"));
    props.setLook(wlMatrixParameters);
    FormData fdlMatrixParameters = new FormData();
    fdlMatrixParameters.left = new FormAttachment(0, 0);
    fdlMatrixParameters.top = new FormAttachment(wTransformName, margin);
    wlMatrixParameters.setLayoutData(fdlMatrixParameters);

    wMatrixGet = new Button(wMatrixParametersComp, SWT.PUSH);
    wMatrixGet.setText(BaseMessages.getString(PKG, "RestDialog.GetParameters.Button"));
    FormData fdMatrixGet = new FormData();
    fdMatrixGet.top = new FormAttachment(wlMatrixParameters, margin);
    fdMatrixGet.right = new FormAttachment(100, 0);
    wMatrixGet.setLayoutData(fdMatrixGet);

    int matrixParametersRows = input.getMatrixParameterField().length;

    colinfoparams =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "RestDialog.ColumnInfo.ParameterField"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RestDialog.ColumnInfo.ParameterName"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };

    wMatrixParameters =
        new TableView(
            variables,
            wMatrixParametersComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinfoparams,
            matrixParametersRows,
            lsMod,
            props);

    FormData fdMatrixParameters = new FormData();
    fdMatrixParameters.left = new FormAttachment(0, 0);
    fdMatrixParameters.top = new FormAttachment(wlMatrixParameters, margin);
    fdMatrixParameters.right = new FormAttachment(wMatrixGet, -margin);
    fdMatrixParameters.bottom = new FormAttachment(100, -margin);
    wMatrixParameters.setLayoutData(fdMatrixParameters);

    FormData fdMatrixParametersComp = new FormData();
    fdMatrixParametersComp.left = new FormAttachment(0, 0);
    fdMatrixParametersComp.top = new FormAttachment(wTransformName, margin);
    fdMatrixParametersComp.right = new FormAttachment(100, 0);
    fdMatrixParametersComp.bottom = new FormAttachment(100, 0);
    wMatrixParametersComp.setLayoutData(fdMatrixParametersComp);

    wMatrixParametersComp.layout();
    wMatrixParametersTab.setControl(wMatrixParametersComp);

    // Add listeners
    wMatrixGet.addListener(SWT.Selection, e -> getParametersFields(wMatrixParameters));

    // END of Matrix Parameters Tab
  }

  private void setupParameterTabContent(ModifyListener lsMod, int margin, Composite wParametersComp) {
    wlParameters = new Label(wParametersComp, SWT.NONE);
    wlParameters.setText(BaseMessages.getString(PKG, "RestDialog.Parameters.Label"));
    props.setLook(wlParameters);
    FormData fdlParameters = new FormData();
    fdlParameters.left = new FormAttachment(0, 0);
    fdlParameters.top = new FormAttachment(wTransformName, margin);
    wlParameters.setLayoutData(fdlParameters);

    wGet = new Button(wParametersComp, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "RestDialog.GetParameters.Button"));
    FormData fdGet = new FormData();
    fdGet.top = new FormAttachment(wlParameters, margin);
    fdGet.right = new FormAttachment(100, 0);
    wGet.setLayoutData(fdGet);

    final int ParametersRows = input.getParameterField().length;

    colinfoparams =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "RestDialog.ColumnInfo.ParameterField"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RestDialog.ColumnInfo.ParameterName"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };

    wParameters =
        new TableView(
            variables,
            wParametersComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinfoparams,
            ParametersRows,
            lsMod,
            props);

    FormData fdParameters = new FormData();
    fdParameters.left = new FormAttachment(0, 0);
    fdParameters.top = new FormAttachment(wlParameters, margin);
    fdParameters.right = new FormAttachment(wGet, -margin);
    fdParameters.bottom = new FormAttachment(100, -margin);
    wParameters.setLayoutData(fdParameters);

    wGet.addListener(SWT.Selection, e -> getParametersFields(wParameters));
  }

  private Button setupHeaderTabContent(ModifyListener lsMod, int margin, Composite wAdditionalComp) {
    Label wlFields = new Label(wAdditionalComp, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "RestDialog.Headers.Label"));
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wTransformName, margin);
    wlFields.setLayoutData(fdlFields);

    Button wGetHeaders = new Button(wAdditionalComp, SWT.PUSH);
    wGetHeaders.setText(BaseMessages.getString(PKG, "RestDialog.GetHeaders.Button"));
    FormData fdGetHeaders = new FormData();
    fdGetHeaders.top = new FormAttachment(wlFields, margin);
    fdGetHeaders.right = new FormAttachment(100, 0);
    wGetHeaders.setLayoutData(fdGetHeaders);

    final int FieldsRows = input.getHeaderName().length;

    colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "RestDialog.ColumnInfo.Field"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "RestDialog.ColumnInfo.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false)
        };

    colinf[1].setUsingVariables(true);
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
    fdFields.right = new FormAttachment(wGetHeaders, -margin);
    fdFields.bottom = new FormAttachment(100, -margin);
    wFields.setLayoutData(fdFields);

    wGetHeaders.addListener(SWT.Selection, e -> getHeaders());

    return wGetHeaders;
  }

  private void setupIgnoreSslLine(int middle, int margin, Group gSSLTrustStore) {
    // Ignore SSL line
    Label wlIgnoreSsl = new Label(gSSLTrustStore, SWT.RIGHT);
    wlIgnoreSsl.setText(BaseMessages.getString(PKG, "RestDialog.IgnoreSsl.Label"));
    props.setLook(wlIgnoreSsl);
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.top = new FormAttachment(wTrustStorePassword, margin);
    fdLabel.right = new FormAttachment(middle, -margin);
    wlIgnoreSsl.setLayoutData(fdLabel);
    wIgnoreSsl = new Button(gSSLTrustStore, SWT.CHECK);
    props.setLook(wIgnoreSsl);
    FormData fdButton = new FormData();
    fdButton.left = new FormAttachment(middle, 0);
    fdButton.top = new FormAttachment(wlIgnoreSsl, 0, SWT.CENTER);
    fdButton.right = new FormAttachment(100, 0);
    wIgnoreSsl.setLayoutData(fdButton);
    wIgnoreSsl.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            activateTrustoreFields();
          }
        });
  }

  private void activateTrustoreFields(){
    wTrustStoreFile.setEnabled(!wIgnoreSsl.getSelection());
    wbTrustStoreFile.setEnabled(!wIgnoreSsl.getSelection());
    wTrustStorePassword.setEnabled(!wIgnoreSsl.getSelection());
  }

  private void setupTrustorePwdLine(ModifyListener lsMod, int middle, int margin, Group gSSLTrustStore, Button wbTrustStoreFile) {
    // TrustStorePassword line
    Label wlTrustStorePassword = new Label(gSSLTrustStore, SWT.RIGHT);
    wlTrustStorePassword.setText(
        BaseMessages.getString(PKG, "RestDialog.TrustStorePassword.Label"));
    props.setLook(wlTrustStorePassword);
    FormData fdlTrustStorePassword = new FormData();
    fdlTrustStorePassword.left = new FormAttachment(0, 0);
    fdlTrustStorePassword.top = new FormAttachment(wbTrustStoreFile, margin);
    fdlTrustStorePassword.right = new FormAttachment(middle, -margin);
    wlTrustStorePassword.setLayoutData(fdlTrustStorePassword);
    wTrustStorePassword =
        new PasswordTextVar(variables, gSSLTrustStore, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTrustStorePassword);
    wTrustStorePassword.addModifyListener(lsMod);
    FormData fdTrustStorePassword = new FormData();
    fdTrustStorePassword.left = new FormAttachment(middle, 0);
    fdTrustStorePassword.top = new FormAttachment(wbTrustStoreFile, margin);
    fdTrustStorePassword.right = new FormAttachment(100, 0);
    wTrustStorePassword.setLayoutData(fdTrustStorePassword);
  }

  private Button setupTrustoreFileLine(ModifyListener lsMod, int middle, int margin, Group gSSLTrustStore) {
    // TrustStoreFile line
    Label wlTrustStoreFile = new Label(gSSLTrustStore, SWT.RIGHT);
    wlTrustStoreFile.setText(BaseMessages.getString(PKG, "RestDialog.TrustStoreFile.Label"));
    props.setLook(wlTrustStoreFile);
    FormData fdlTrustStoreFile = new FormData();
    fdlTrustStoreFile.left = new FormAttachment(0, 0);
    fdlTrustStoreFile.top = new FormAttachment(0, margin);
    fdlTrustStoreFile.right = new FormAttachment(middle, -margin);
    wlTrustStoreFile.setLayoutData(fdlTrustStoreFile);

    wbTrustStoreFile = new Button(gSSLTrustStore, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTrustStoreFile);
    wbTrustStoreFile.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbTrustStoreFile = new FormData();
    fdbTrustStoreFile.right = new FormAttachment(100, 0);
    fdbTrustStoreFile.top = new FormAttachment(0, 0);
    wbTrustStoreFile.setLayoutData(fdbTrustStoreFile);

    wbTrustStoreFile.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell,
                wTrustStoreFile,
                variables,
                new String[] {"*.)"},
                new String[] {BaseMessages.getString(PKG, "System.FileType.AllFiles")},
                true));

    wTrustStoreFile = new TextVar(variables, gSSLTrustStore, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTrustStoreFile);
    wTrustStoreFile.addModifyListener(lsMod);
    FormData fdTrustStoreFile = new FormData();
    fdTrustStoreFile.left = new FormAttachment(middle, 0);
    fdTrustStoreFile.top = new FormAttachment(0, margin);
    fdTrustStoreFile.right = new FormAttachment(wbTrustStoreFile, -margin);
    wTrustStoreFile.setLayoutData(fdTrustStoreFile);
    return wbTrustStoreFile;
  }

  private Group setupTrustoreGroup(Composite wSSLComp) {
    Group gSSLTrustStore = new Group(wSSLComp, SWT.SHADOW_ETCHED_IN);
    gSSLTrustStore.setText(BaseMessages.getString(PKG, "RestDialog.SSLTrustStoreGroup.Label"));
    FormLayout sslTrustStoreLayout = new FormLayout();
    sslTrustStoreLayout.marginWidth = 3;
    sslTrustStoreLayout.marginHeight = 3;
    gSSLTrustStore.setLayout(sslTrustStoreLayout);
    props.setLook(gSSLTrustStore);
    return gSSLTrustStore;
  }

  private void setupProxyPortLine(ModifyListener lsMod, int middle, int margin, Group gProxy) {
    // Proxy Port
    Label wlProxyPort = new Label(gProxy, SWT.RIGHT);
    wlProxyPort.setText(BaseMessages.getString(PKG, "RestDialog.ProxyPort.Label"));
    props.setLook(wlProxyPort);
    FormData fdlProxyPort = new FormData();
    fdlProxyPort.top = new FormAttachment(wProxyHost, margin);
    fdlProxyPort.left = new FormAttachment(0, 0);
    fdlProxyPort.right = new FormAttachment(middle, -margin);
    wlProxyPort.setLayoutData(fdlProxyPort);
    wProxyPort = new TextVar(variables, gProxy, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wProxyPort.addModifyListener(lsMod);
    wProxyPort.setToolTipText(BaseMessages.getString(PKG, "RestDialog.ProxyPort.Tooltip"));
    props.setLook(wProxyPort);
    FormData fdProxyPort = new FormData();
    fdProxyPort.top = new FormAttachment(wProxyHost, margin);
    fdProxyPort.left = new FormAttachment(middle, 0);
    fdProxyPort.right = new FormAttachment(100, 0);
    wProxyPort.setLayoutData(fdProxyPort);
  }

  private void setupProxyHostLine(ModifyListener lsMod, int middle, int margin, Group gProxy) {
    // Proxy host
    Label wlProxyHost = new Label(gProxy, SWT.RIGHT);
    wlProxyHost.setText(BaseMessages.getString(PKG, "RestDialog.ProxyHost.Label"));
    props.setLook(wlProxyHost);
    FormData fdlProxyHost = new FormData();
    fdlProxyHost.top = new FormAttachment(0, margin);
    fdlProxyHost.left = new FormAttachment(0, 0);
    fdlProxyHost.right = new FormAttachment(middle, -margin);
    wlProxyHost.setLayoutData(fdlProxyHost);
    wProxyHost = new TextVar(variables, gProxy, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wProxyHost.addModifyListener(lsMod);
    wProxyHost.setToolTipText(BaseMessages.getString(PKG, "RestDialog.ProxyHost.Tooltip"));
    props.setLook(wProxyHost);
    FormData fdProxyHost = new FormData();
    fdProxyHost.top = new FormAttachment(0, margin);
    fdProxyHost.left = new FormAttachment(middle, 0);
    fdProxyHost.right = new FormAttachment(100, 0);
    wProxyHost.setLayoutData(fdProxyHost);
  }

  private Group setupProxyGroup(Composite wAuthComp) {
    Group gProxy = new Group(wAuthComp, SWT.SHADOW_ETCHED_IN);
    gProxy.setText(BaseMessages.getString(PKG, "RestDialog.ProxyGroup.Label"));
    FormLayout proxyLayout = new FormLayout();
    proxyLayout.marginWidth = 3;
    proxyLayout.marginHeight = 3;
    gProxy.setLayout(proxyLayout);
    props.setLook(gProxy);
    return gProxy;
  }

  private void setupPreemptiveLine(int middle, int margin, Group gHttpAuth) {
    // Preemptive line
    Label wlPreemptive = new Label(gHttpAuth, SWT.RIGHT);
    wlPreemptive.setText(BaseMessages.getString(PKG, "RestDialog.Preemptive.Label"));
    props.setLook(wlPreemptive);
    FormData fdlPreemptive = new FormData();
    fdlPreemptive.left = new FormAttachment(0, 0);
    fdlPreemptive.top = new FormAttachment(wHttpPassword, margin);
    fdlPreemptive.right = new FormAttachment(middle, -margin);
    wlPreemptive.setLayoutData(fdlPreemptive);
    wPreemptive = new Button(gHttpAuth, SWT.CHECK);
    props.setLook(wPreemptive);
    FormData fdPreemptive = new FormData();
    fdPreemptive.left = new FormAttachment(middle, 0);
    fdPreemptive.top = new FormAttachment(wlPreemptive, 0, SWT.CENTER);
    fdPreemptive.right = new FormAttachment(100, 0);
    wPreemptive.setLayoutData(fdPreemptive);
    wPreemptive.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });
  }

  private void setupPasswordLine(ModifyListener lsMod, int middle, int margin, Group gHttpAuth) {
    // HTTP Password
    Label wlHttpPassword = new Label(gHttpAuth, SWT.RIGHT);
    wlHttpPassword.setText(BaseMessages.getString(PKG, "RestDialog.HttpPassword.Label"));
    props.setLook(wlHttpPassword);
    FormData fdlHttpPassword = new FormData();
    fdlHttpPassword.top = new FormAttachment(wHttpLogin, margin);
    fdlHttpPassword.left = new FormAttachment(0, 0);
    fdlHttpPassword.right = new FormAttachment(middle, -margin);
    wlHttpPassword.setLayoutData(fdlHttpPassword);
    wHttpPassword = new PasswordTextVar(variables, gHttpAuth, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wHttpPassword.addModifyListener(lsMod);
    wHttpPassword.setToolTipText(BaseMessages.getString(PKG, "RestDialog.HttpPassword.Tooltip"));
    props.setLook(wHttpPassword);
    FormData fdHttpPassword = new FormData();
    fdHttpPassword.top = new FormAttachment(wHttpLogin, margin);
    fdHttpPassword.left = new FormAttachment(middle, 0);
    fdHttpPassword.right = new FormAttachment(100, 0);
    wHttpPassword.setLayoutData(fdHttpPassword);
  }

  private void setupLoginLine(ModifyListener lsMod, int middle, int margin, Group gHttpAuth) {
    // HTTP Login
    Label wlHttpLogin = new Label(gHttpAuth, SWT.RIGHT);
    wlHttpLogin.setText(BaseMessages.getString(PKG, "RestDialog.HttpLogin.Label"));
    props.setLook(wlHttpLogin);
    FormData fdlHttpLogin = new FormData();
    fdlHttpLogin.top = new FormAttachment(0, margin);
    fdlHttpLogin.left = new FormAttachment(0, 0);
    fdlHttpLogin.right = new FormAttachment(middle, -margin);
    wlHttpLogin.setLayoutData(fdlHttpLogin);
    wHttpLogin = new TextVar(variables, gHttpAuth, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wHttpLogin.addModifyListener(lsMod);
    wHttpLogin.setToolTipText(BaseMessages.getString(PKG, "RestDialog.HttpLogin.Tooltip"));
    props.setLook(wHttpLogin);
    FormData fdHttpLogin = new FormData();
    fdHttpLogin.top = new FormAttachment(0, margin);
    fdHttpLogin.left = new FormAttachment(middle, 0);
    fdHttpLogin.right = new FormAttachment(100, 0);
    wHttpLogin.setLayoutData(fdHttpLogin);
  }

  private Group setupAuthGroup(Composite wAuthComp) {
    Group gHttpAuth = new Group(wAuthComp, SWT.SHADOW_ETCHED_IN);
    gHttpAuth.setText(BaseMessages.getString(PKG, "RestDialog.HttpAuthGroup.Label"));
    FormLayout httpAuthLayout = new FormLayout();
    httpAuthLayout.marginWidth = 3;
    httpAuthLayout.marginHeight = 3;
    gHttpAuth.setLayout(httpAuthLayout);
    props.setLook(gHttpAuth);
    return gHttpAuth;
  }

  private void setupResponseHeaderLine(ModifyListener lsMod, int middle, int margin, Group gSettings, Group gOutputFields) {
    // Response header line...
    Label wlResponseHeader = new Label(gOutputFields, SWT.RIGHT);
    wlResponseHeader.setText(BaseMessages.getString(PKG, "RestDialog.ResponseHeader.Label"));
    props.setLook(wlResponseHeader);
    FormData fdlResponseHeader = new FormData();
    fdlResponseHeader.left = new FormAttachment(0, 0);
    fdlResponseHeader.right = new FormAttachment(middle, -margin);
    fdlResponseHeader.top = new FormAttachment(wResponseTime, margin);
    wlResponseHeader.setLayoutData(fdlResponseHeader);
    wResponseHeader = new TextVar(variables, gOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wResponseHeader);
    wResponseHeader.addModifyListener(lsMod);
    FormData fdResponseHeader = new FormData();
    fdResponseHeader.left = new FormAttachment(middle, 0);
    fdResponseHeader.top = new FormAttachment(wResponseTime, margin);
    fdResponseHeader.right = new FormAttachment(100, 0);
    wResponseHeader.setLayoutData(fdResponseHeader);

    FormData fdOutputFields = new FormData();
    fdOutputFields.left = new FormAttachment(0, 0);
    fdOutputFields.right = new FormAttachment(100, 0);
    fdOutputFields.top = new FormAttachment(gSettings, margin);
    gOutputFields.setLayoutData(fdOutputFields);
  }

  private void setupResponseTimeLine(ModifyListener lsMod, int middle, int margin, Group gOutputFields) {
    // Response time line...
    Label wlResponseTime = new Label(gOutputFields, SWT.RIGHT);
    wlResponseTime.setText(BaseMessages.getString(PKG, "RestDialog.ResponseTime.Label"));
    props.setLook(wlResponseTime);
    FormData fdlResponseTime = new FormData();
    fdlResponseTime.left = new FormAttachment(0, 0);
    fdlResponseTime.right = new FormAttachment(middle, -margin);
    fdlResponseTime.top = new FormAttachment(wResultCode, margin);
    wlResponseTime.setLayoutData(fdlResponseTime);
    wResponseTime = new TextVar(variables, gOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wResponseTime);
    wResponseTime.addModifyListener(lsMod);
    FormData fdResponseTime = new FormData();
    fdResponseTime.left = new FormAttachment(middle, 0);
    fdResponseTime.top = new FormAttachment(wResultCode, margin);
    fdResponseTime.right = new FormAttachment(100, 0);
    wResponseTime.setLayoutData(fdResponseTime);
  }

  private void setupStatusCodeLine(ModifyListener lsMod, int middle, int margin, Group gOutputFields) {
    // Resultcode line...
    Label wlResultCode = new Label(gOutputFields, SWT.RIGHT);
    wlResultCode.setText(BaseMessages.getString(PKG, "RestDialog.ResultCode.Label"));
    props.setLook(wlResultCode);
    FormData fdlResultCode = new FormData();
    fdlResultCode.left = new FormAttachment(0, 0);
    fdlResultCode.right = new FormAttachment(middle, -margin);
    fdlResultCode.top = new FormAttachment(wResult, margin);
    wlResultCode.setLayoutData(fdlResultCode);
    wResultCode = new TextVar(variables, gOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wResultCode);
    wResultCode.addModifyListener(lsMod);
    FormData fdResultCode = new FormData();
    fdResultCode.left = new FormAttachment(middle, 0);
    fdResultCode.top = new FormAttachment(wResult, margin);
    fdResultCode.right = new FormAttachment(100, -margin);
    wResultCode.setLayoutData(fdResultCode);
  }

  private void setupResultLine(ModifyListener lsMod, int middle, int margin, Group gSettings, Group gOutputFields) {
    // Result line...
    Label wlResult = new Label(gOutputFields, SWT.RIGHT);
    wlResult.setText(BaseMessages.getString(PKG, "RestDialog.Result.Label"));
    props.setLook(wlResult);
    FormData fdlResult = new FormData();
    fdlResult.left = new FormAttachment(0, 0);
    fdlResult.right = new FormAttachment(middle, -margin);
    fdlResult.top = new FormAttachment(gSettings, margin);
    wlResult.setLayoutData(fdlResult);
    wResult = new TextVar(variables, gOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wResult);
    wResult.addModifyListener(lsMod);
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment(middle, 0);
    fdResult.top = new FormAttachment(gSettings, margin * 2);
    fdResult.right = new FormAttachment(100, -margin);
    wResult.setLayoutData(fdResult);
  }

  private Group setupOutputFieldGroup(Composite wGeneralComp) {
    Group gOutputFields = new Group(wGeneralComp, SWT.SHADOW_ETCHED_IN);
    gOutputFields.setText(BaseMessages.getString(PKG, "RestDialog.OutputFieldsGroup.Label"));
    FormLayout outputFieldsLayout = new FormLayout();
    outputFieldsLayout.marginWidth = 3;
    outputFieldsLayout.marginHeight = 3;
    gOutputFields.setLayout(outputFieldsLayout);
    props.setLook(gOutputFields);
    return gOutputFields;
  }

  private void setupAppTypeLine(ModifyListener lsMod, int middle, int margin, Group gSettings) {
    // ApplicationType Line
    Label wlApplicationType = new Label(gSettings, SWT.RIGHT);
    wlApplicationType.setText(BaseMessages.getString(PKG, "RestDialog.ApplicationType.Label"));
    props.setLook(wlApplicationType);
    FormData fdlApplicationType = new FormData();
    fdlApplicationType.left = new FormAttachment(0, 0);
    fdlApplicationType.right = new FormAttachment(middle, -margin);
    fdlApplicationType.top = new FormAttachment(wBody, 2 * margin);
    wlApplicationType.setLayoutData(fdlApplicationType);

    wApplicationType = new ComboVar(variables, gSettings, SWT.BORDER | SWT.READ_ONLY);
    wApplicationType.setEditable(true);
    props.setLook(wApplicationType);
    wApplicationType.addModifyListener(lsMod);
    FormData fdApplicationType = new FormData();
    fdApplicationType.left = new FormAttachment(middle, 0);
    fdApplicationType.top = new FormAttachment(wBody, 2 * margin);
    fdApplicationType.right = new FormAttachment(100, -margin);
    wApplicationType.setLayoutData(fdApplicationType);
    wApplicationType.setItems(RestMeta.APPLICATION_TYPES);
    wApplicationType.addSelectionListener(
        new SelectionAdapter() {

          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });
  }

  private void setupBodyLine(ModifyListener lsMod, int middle, int margin, Group gSettings) {
    // Body Line
    wlBody = new Label(gSettings, SWT.RIGHT);
    wlBody.setText(BaseMessages.getString(PKG, "RestDialog.Body.Label"));
    props.setLook(wlBody);
    FormData fdlBody = new FormData();
    fdlBody.left = new FormAttachment(0, 0);
    fdlBody.right = new FormAttachment(middle, -margin);
    fdlBody.top = new FormAttachment(wMethodField, 2 * margin);
    wlBody.setLayoutData(fdlBody);

    wBody = new ComboVar(variables, gSettings, SWT.BORDER | SWT.READ_ONLY);
    wBody.setEditable(true);
    props.setLook(wBody);
    wBody.addModifyListener(lsMod);
    FormData fdBody = new FormData();
    fdBody.left = new FormAttachment(middle, 0);
    fdBody.top = new FormAttachment(wMethodField, 2 * margin);
    fdBody.right = new FormAttachment(100, -margin);
    wBody.setLayoutData(fdBody);
    wBody.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {}

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

  private void setupMethodNameLine(ModifyListener lsMod, int middle, int margin, Group gSettings) {
    // MethodField Line
    wlMethodField = new Label(gSettings, SWT.RIGHT);
    wlMethodField.setText(BaseMessages.getString(PKG, "RestDialog.MethodField.Label"));
    props.setLook(wlMethodField);
    FormData fdlMethodField = new FormData();
    fdlMethodField.left = new FormAttachment(0, 0);
    fdlMethodField.right = new FormAttachment(middle, -margin);
    fdlMethodField.top = new FormAttachment(wMethodInField, margin);
    wlMethodField.setLayoutData(fdlMethodField);

    wMethodField = new ComboVar(variables, gSettings, SWT.BORDER | SWT.READ_ONLY);
    wMethodField.setEditable(true);
    props.setLook(wMethodField);
    wMethodField.addModifyListener(lsMod);
    FormData fdMethodField = new FormData();
    fdMethodField.left = new FormAttachment(middle, 0);
    fdMethodField.top = new FormAttachment(wMethodInField, margin);
    fdMethodField.right = new FormAttachment(100, -margin);
    wMethodField.setLayoutData(fdMethodField);
    wMethodField.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {}

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

  private void setupMethodInFieldLine(int middle, int margin, Group gSettings) {
    // MethodInField line
    Label wlMethodInField = new Label(gSettings, SWT.RIGHT);
    wlMethodInField.setText(BaseMessages.getString(PKG, "RestDialog.MethodInField.Label"));
    props.setLook(wlMethodInField);
    FormData fdlMethodInField = new FormData();
    fdlMethodInField.left = new FormAttachment(0, 0);
    fdlMethodInField.top = new FormAttachment(wMethod, margin);
    fdlMethodInField.right = new FormAttachment(middle, -margin);
    wlMethodInField.setLayoutData(fdlMethodInField);
    wMethodInField = new Button(gSettings, SWT.CHECK);
    props.setLook(wMethodInField);
    FormData fdMethodInField = new FormData();
    fdMethodInField.left = new FormAttachment(middle, 0);
    fdMethodInField.top = new FormAttachment(wlMethodInField, 0, SWT.CENTER);
    fdMethodInField.right = new FormAttachment(100, 0);
    wMethodInField.setLayoutData(fdMethodInField);
    wMethodInField.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            activateMethodInfield();
          }
        });
  }

  private void setupHttpMethodLine(ModifyListener lsMod, int middle, int margin, Group gSettings) {
    // Method Line
    wlMethod = new Label(gSettings, SWT.RIGHT);
    wlMethod.setText(BaseMessages.getString(PKG, "RestDialog.Method.Label"));
    props.setLook(wlMethod);
    FormData fdlMethod = new FormData();
    fdlMethod.left = new FormAttachment(0, 0);
    fdlMethod.right = new FormAttachment(middle, -margin);
    fdlMethod.top = new FormAttachment(wUrlField, 2 * margin);
    wlMethod.setLayoutData(fdlMethod);

    wMethod = new ComboVar(variables, gSettings, SWT.BORDER | SWT.READ_ONLY);
    wMethod.setEditable(true);
    props.setLook(wMethod);
    wMethod.addModifyListener(lsMod);
    FormData fdMethod = new FormData();
    fdMethod.left = new FormAttachment(middle, 0);
    fdMethod.top = new FormAttachment(wUrlField, 2 * margin);
    fdMethod.right = new FormAttachment(100, -margin);
    wMethod.setLayoutData(fdMethod);
    wMethod.setItems(RestMeta.HTTP_METHODS);
    wMethod.addSelectionListener(
        new SelectionAdapter() {

          @Override
          public void widgetSelected(SelectionEvent e) {
            setMethod();
          }
        });
  }

  private void setupUrlFieldNameLine(ModifyListener lsMod, int middle, int margin, Group gSettings) {
    // UrlField Line
    wlUrlField = new Label(gSettings, SWT.RIGHT);
    wlUrlField.setText(BaseMessages.getString(PKG, "RestDialog.UrlField.Label"));
    props.setLook(wlUrlField);
    FormData fdlUrlField = new FormData();
    fdlUrlField.left = new FormAttachment(0, 0);
    fdlUrlField.right = new FormAttachment(middle, -margin);
    fdlUrlField.top = new FormAttachment(wUrlInField, margin);
    wlUrlField.setLayoutData(fdlUrlField);

    wUrlField = new ComboVar(variables, gSettings, SWT.BORDER | SWT.READ_ONLY);
    wUrlField.setEditable(true);
    props.setLook(wUrlField);
    wUrlField.addModifyListener(lsMod);
    FormData fdUrlField = new FormData();
    fdUrlField.left = new FormAttachment(middle, 0);
    fdUrlField.top = new FormAttachment(wUrlInField, margin);
    fdUrlField.right = new FormAttachment(100, -margin);
    wUrlField.setLayoutData(fdUrlField);
    wUrlField.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {}

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

  private void setupUrlInFieldLine(int middle, int margin, Group gSettings) {
    // UrlInField line
    Label wlUrlInField = new Label(gSettings, SWT.RIGHT);
    wlUrlInField.setText(BaseMessages.getString(PKG, "RestDialog.UrlInField.Label"));
    props.setLook(wlUrlInField);
    FormData fdlUrlInField = new FormData();
    fdlUrlInField.left = new FormAttachment(0, 0);
    fdlUrlInField.top = new FormAttachment(wUrl, margin);
    fdlUrlInField.right = new FormAttachment(middle, -margin);
    wlUrlInField.setLayoutData(fdlUrlInField);
    wUrlInField = new Button(gSettings, SWT.CHECK);
    props.setLook(wUrlInField);
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
            activateUrlInfield();
          }
        });
  }

  private void setupUrlLine(ModifyListener lsMod, int middle, int margin, Composite wGeneralComp, Group gSettings) {
    wlUrl = new Label(gSettings, SWT.RIGHT);
    wlUrl.setText(BaseMessages.getString(PKG, "RestDialog.URL.Label"));
    props.setLook(wlUrl);
    FormData fdlUrl = new FormData();
    fdlUrl.left = new FormAttachment(0, 0);
    fdlUrl.right = new FormAttachment(middle, -margin);
    fdlUrl.top = new FormAttachment(wGeneralComp, margin * 2);
    wlUrl.setLayoutData(fdlUrl);

    wUrl = new TextVar(variables, gSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wUrl);
    wUrl.addModifyListener(lsMod);
    FormData fdUrl = new FormData();
    fdUrl.left = new FormAttachment(middle, 0);
    fdUrl.top = new FormAttachment(wGeneralComp, margin * 2);
    fdUrl.right = new FormAttachment(100, 0);
    wUrl.setLayoutData(fdUrl);
  }

  private Group setupSettingGroup(Composite wGeneralComp) {
    Group gSettings = new Group(wGeneralComp, SWT.SHADOW_ETCHED_IN);
    gSettings.setText(BaseMessages.getString(PKG, "RestDialog.SettingsGroup.Label"));
    FormLayout settingsLayout = new FormLayout();
    settingsLayout.marginWidth = 3;
    settingsLayout.marginHeight = 3;
    gSettings.setLayout(settingsLayout);
    props.setLook(gSettings);
    return gSettings;
  }

  private void setupTransformName(ModifyListener lsMod, int middle, int margin) {
    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "RestDialog.TransformName.Label"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
  }

  private void setupButtons(int margin) {
    // THE BUTTONS: at the bottom
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);
  }

  protected void setMethod() {
    boolean activateBody = RestMeta.isActiveBody(wMethod.getText());
    boolean activateParams = RestMeta.isActiveParameters(wMethod.getText());

    wlBody.setEnabled(activateBody);
    wBody.setEnabled(activateBody);
    wApplicationType.setEnabled(activateBody);

    wlParameters.setEnabled(activateParams);
    wParameters.setEnabled(activateParams);
    wGet.setEnabled(activateParams);
    wlMatrixParameters.setEnabled(activateParams);
    wMatrixParameters.setEnabled(activateParams);
    wMatrixGet.setEnabled(activateParams);
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll(inputFields);

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>(keySet);

    fieldNames = entries.toArray(new String[entries.size()]);

    Const.sortStrings(fieldNames);
    colinfoparams[0].setComboValues(fieldNames);
    colinf[0].setComboValues(fieldNames);
  }

  private void setStreamFields() {
    if (!gotPreviousFields) {
      String urlfield = wUrlField.getText();
      String body = wBody.getText();
      String method = wMethodField.getText();

      wUrlField.removeAll();
      wBody.removeAll();
      wMethodField.removeAll();

      try {
        if (fieldNames != null) {
          wUrlField.setItems(fieldNames);
          wBody.setItems(fieldNames);
          wMethodField.setItems(fieldNames);
        }
      } finally {
        if (urlfield != null) {
          wUrlField.setText(urlfield);
        }
        if (body != null) {
          wBody.setText(body);
        }
        if (method != null) {
          wMethodField.setText(method);
        }
      }
      gotPreviousFields = true;
    }
  }

  private void activateUrlInfield() {
    wlUrlField.setEnabled(wUrlInField.getSelection());
    wUrlField.setEnabled(wUrlInField.getSelection());
    wlUrl.setEnabled(!wUrlInField.getSelection());
    wUrl.setEnabled(!wUrlInField.getSelection());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (isDebug()) {
      logDebug(BaseMessages.getString(PKG, "RestDialog.Log.GettingKeyInfo"));
    }

    if (input.getHeaderName() != null) {
      for (int i = 0; i < input.getHeaderName().length; i++) {
        TableItem item = wFields.table.getItem(i);
        if (input.getHeaderField()[i] != null) {
          item.setText(1, input.getHeaderField()[i]);
        }
        if (input.getHeaderName()[i] != null) {
          item.setText(2, input.getHeaderName()[i]);
        }
      }
    }

    if (input.getParameterField() != null) {
      for (int i = 0; i < input.getParameterField().length; i++) {
        TableItem item = wParameters.table.getItem(i);
        if (input.getParameterField()[i] != null) {
          item.setText(1, input.getParameterField()[i]);
        }
        if (input.getParameterName()[i] != null) {
          item.setText(2, input.getParameterName()[i]);
        }
      }
    }

    if (input.getMatrixParameterField() != null) {
      for (int i = 0; i < input.getMatrixParameterField().length; i++) {
        TableItem item = wMatrixParameters.table.getItem(i);
        if (input.getMatrixParameterField()[i] != null) {
          item.setText(1, input.getMatrixParameterField()[i]);
        }
        if (input.getMatrixParameterField()[i] != null) {
          item.setText(2, input.getMatrixParameterField()[i]);
        }
      }
    }

    wMethod.setText(Const.NVL(input.getMethod(), RestMeta.HTTP_METHOD_GET));
    wMethodInField.setSelection(input.isDynamicMethod());
    if (input.getBodyField() != null) {
      wBody.setText(input.getBodyField());
    }
    if (input.getMethodFieldName() != null) {
      wMethodField.setText(input.getMethodFieldName());
    }
    if (input.getUrl() != null) {
      wUrl.setText(input.getUrl());
    }
    wUrlInField.setSelection(input.isUrlInField());
    if (input.getUrlField() != null) {
      wUrlField.setText(input.getUrlField());
    }
    if (input.getFieldName() != null) {
      wResult.setText(input.getFieldName());
    }
    if (input.getResultCodeFieldName() != null) {
      wResultCode.setText(input.getResultCodeFieldName());
    }
    if (input.getResponseTimeFieldName() != null) {
      wResponseTime.setText(input.getResponseTimeFieldName());
    }

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
    wPreemptive.setSelection(input.isPreemptive());

    if (input.getTrustStoreFile() != null) {
      wTrustStoreFile.setText(input.getTrustStoreFile());
    }
    if (input.getTrustStorePassword() != null) {
      wTrustStorePassword.setText(input.getTrustStorePassword());
    }
    wIgnoreSsl.setSelection(input.isIgnoreSsl());
    if (input.getResponseHeaderFieldName() != null) {
      wResponseHeader.setText(input.getResponseHeaderFieldName());
    }

    wApplicationType.setText(Const.NVL(input.getApplicationType(), ""));

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
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    int nrheaders = wFields.nrNonEmpty();
    int nrparams = wParameters.nrNonEmpty();
    int nrmatrixparams = wMatrixParameters.nrNonEmpty();
    input.allocate(nrheaders, nrparams, nrmatrixparams);

    if (isDebug()) {
      logDebug(
          BaseMessages.getString(PKG, "RestDialog.Log.FoundArguments", String.valueOf(nrheaders)));
    }
    for (int i = 0; i < nrheaders; i++) {
      TableItem item = wFields.getNonEmpty(i);
      input.getHeaderField()[i] = item.getText(1);
      input.getHeaderName()[i] = item.getText(2);
    }
    for (int i = 0; i < nrparams; i++) {
      TableItem item = wParameters.getNonEmpty(i);
      input.getParameterField()[i] = item.getText(1);
      input.getParameterName()[i] = item.getText(2);
    }

    for (int i = 0; i < nrmatrixparams; i++) {
      TableItem item = wMatrixParameters.getNonEmpty(i);
      input.getMatrixParameterField()[i] = item.getText(1);
      input.getMatrixParameterName()[i] = item.getText(2);
    }

    input.setDynamicMethod(wMethodInField.getSelection());
    input.setMethodFieldName(wMethodField.getText());
    input.setMethod(wMethod.getText());
    input.setUrl(wUrl.getText());
    input.setUrlField(wUrlField.getText());
    input.setUrlInField(wUrlInField.getSelection());
    input.setBodyField(wBody.getText());
    input.setFieldName(wResult.getText());
    input.setResultCodeFieldName(wResultCode.getText());
    input.setResponseTimeFieldName(wResponseTime.getText());
    input.setResponseHeaderFieldName(wResponseHeader.getText());

    input.setHttpLogin(wHttpLogin.getText());
    input.setHttpPassword(wHttpPassword.getText());
    input.setProxyHost(wProxyHost.getText());
    input.setProxyPort(wProxyPort.getText());
    input.setPreemptive(wPreemptive.getSelection());

    input.setTrustStoreFile(wTrustStoreFile.getText());
    input.setTrustStorePassword(wTrustStorePassword.getText());
    input.setIgnoreSsl(wIgnoreSsl.getSelection());
    input.setApplicationType(wApplicationType.getText());
    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void getParametersFields(TableView tView) {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, tView, 1, new int[] {1, 2}, new int[] {3}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "RestDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "RestDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  private void getHeaders() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wFields, 1, new int[] {1, 2}, new int[] {3}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "RestDialog.FailedToGetHeaders.DialogTitle"),
          BaseMessages.getString(PKG, "RestDialog.FailedToGetHeaders.DialogMessage"),
          ke);
    }
  }

  private void activateMethodInfield() {
    wlMethod.setEnabled(!wMethodInField.getSelection());
    wMethod.setEnabled(!wMethodInField.getSelection());
    wlMethodField.setEnabled(wMethodInField.getSelection());
    wMethodField.setEnabled(wMethodInField.getSelection());
  }
}
