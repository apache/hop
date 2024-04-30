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
package org.apache.hop.pipeline.transforms.googlesheets;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class GoogleSheetsOutputDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = GoogleSheetsOutputMeta.class; // for Translator

  private final GoogleSheetsOutputMeta meta;
  private Label wlTestServiceAccountInfo;
  private TextVar wPrivateKeyStore;
  private TextVar wSpreadsheetKey;
  private TextVar wWorksheetId;
  private TextVar wShareEmail;
  private TextVar wShareDomainWise;
  private Button wbCreate;
  private Button wbAppend;
  private TextVar wTimeout;
  private TextVar wImpersonation;
  private TextVar wAppName;

  public GoogleSheetsOutputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String name) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, name);
    this.meta = (GoogleSheetsOutputMeta) in;
  }

  private static HttpRequestInitializer setHttpTimeout(
      final HttpRequestInitializer requestInitializer, final String timeout) {
    return new HttpRequestInitializer() {
      @Override
      public void initialize(HttpRequest httpRequest) throws IOException {
        requestInitializer.initialize(httpRequest);
        Integer TO = 5;
        if (!timeout.isEmpty()) {
          TO = Integer.parseInt(timeout);
        }

        httpRequest.setConnectTimeout(TO * 60000); // 3 minutes connect timeout
        httpRequest.setReadTimeout(TO * 60000); // 3 minutes read timeout
      }
    };
  }

  @Override
  public String open() {
    Shell parent = this.getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, meta);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "GoogleSheetsOutput.transform.Name"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // OK and cancel buttons at the bottom
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    // transformName - Label
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "GoogleSheetsOutput.transform.Name"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);

    // transformName - Text
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    CTabFolder tabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(tabFolder, Props.WIDGET_STYLE_TAB);
    tabFolder.setSimple(false);

    /*
     * BEGIN Service Account Tab
     */
    CTabItem serviceAccountTab = new CTabItem(tabFolder, SWT.NONE);
    serviceAccountTab.setText(BaseMessages.getString(PKG, "GoogleSheetsDialog.Tab.ServiceAccount"));

    Composite serviceAccountComposite = new Composite(tabFolder, SWT.NONE);
    props.setLook(serviceAccountComposite);

    FormLayout serviceAccountLayout = new FormLayout();
    serviceAccountLayout.marginWidth = 3;
    serviceAccountLayout.marginHeight = 3;
    serviceAccountComposite.setLayout(serviceAccountLayout);

    // privateKey json - Label
    Label wlPrivateKey = new Label(serviceAccountComposite, SWT.RIGHT);
    wlPrivateKey.setText(BaseMessages.getString(PKG, "GoogleSheetsDialog.PrivateKeyStore"));
    props.setLook(wlPrivateKey);
    FormData fdlPrivateKey = new FormData();
    fdlPrivateKey.top = new FormAttachment(0, margin);
    fdlPrivateKey.left = new FormAttachment(0, 0);
    fdlPrivateKey.right = new FormAttachment(middle, -margin);
    wlPrivateKey.setLayoutData(fdlPrivateKey);

    // privateKey - Button
    Button wbPrivateKey = new Button(serviceAccountComposite, SWT.PUSH | SWT.CENTER);
    props.setLook(wbPrivateKey);
    wbPrivateKey.setText(BaseMessages.getString("System.Button.Browse"));
    FormData fdbPrivateKey = new FormData();
    fdbPrivateKey.top = new FormAttachment(0, margin);
    fdbPrivateKey.right = new FormAttachment(100, 0);
    wbPrivateKey.setLayoutData(fdbPrivateKey);
    wbPrivateKey.addListener(SWT.Selection, e -> selectPrivateKeyFile());

    // privatekey - Text
    wPrivateKeyStore =
        new TextVar(variables, serviceAccountComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPrivateKeyStore);
    FormData fdPrivateKey = new FormData();
    fdPrivateKey.top = new FormAttachment(0, margin);
    fdPrivateKey.left = new FormAttachment(middle, 0);
    fdPrivateKey.right = new FormAttachment(wbPrivateKey, -margin);
    wPrivateKeyStore.setLayoutData(fdPrivateKey);

    // Appname - Label
    Label appNameLabel = new Label(serviceAccountComposite, SWT.RIGHT);
    appNameLabel.setText("Google Application Name :");
    props.setLook(appNameLabel);
    FormData appNameLabelForm = new FormData();
    appNameLabelForm.top = new FormAttachment(wbPrivateKey, margin);
    appNameLabelForm.left = new FormAttachment(0, 0);
    appNameLabelForm.right = new FormAttachment(middle, -margin);
    appNameLabel.setLayoutData(appNameLabelForm);

    // Appname - Text
    wAppName = new TextVar(variables, serviceAccountComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wAppName);
    //    wAppName.addModifyListener(modifiedListener);
    FormData appNameData = new FormData();
    appNameData.top = new FormAttachment(wbPrivateKey, margin);
    appNameData.left = new FormAttachment(middle, 0);
    appNameData.right = new FormAttachment(wbPrivateKey, -margin);
    wAppName.setLayoutData(appNameData);

    // Timeout - Label
    Label timeoutLabel = new Label(serviceAccountComposite, SWT.RIGHT);
    timeoutLabel.setText("Time out in minutes :");
    props.setLook(timeoutLabel);
    FormData timeoutLabelForm = new FormData();
    timeoutLabelForm.top = new FormAttachment(appNameLabel, margin);
    timeoutLabelForm.left = new FormAttachment(0, 0);
    timeoutLabelForm.right = new FormAttachment(middle, -margin);
    timeoutLabel.setLayoutData(timeoutLabelForm);

    // timeout - Text
    wTimeout = new TextVar(variables, serviceAccountComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTimeout);
    FormData timeoutData = new FormData();
    timeoutData.top = new FormAttachment(appNameLabel, margin);
    timeoutData.left = new FormAttachment(middle, 0);
    timeoutData.right = new FormAttachment(wbPrivateKey, -margin);
    wTimeout.setLayoutData(timeoutData);

    // Impersonation - Label
    Label impersonationLabel = new Label(serviceAccountComposite, SWT.RIGHT);
    impersonationLabel.setText(
        BaseMessages.getString(PKG, "GoogleSheetsOutputDialog.ImpersonationAccount"));
    props.setLook(impersonationLabel);
    FormData impersonationLabelForm = new FormData();
    impersonationLabelForm.top = new FormAttachment(wTimeout, margin);
    impersonationLabelForm.left = new FormAttachment(0, 0);
    impersonationLabelForm.right = new FormAttachment(middle, -margin);
    impersonationLabel.setLayoutData(impersonationLabelForm);

    // impersonation - Text
    wImpersonation =
        new TextVar(variables, serviceAccountComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wImpersonation);
    FormData impersonationData = new FormData();
    impersonationData.top = new FormAttachment(wTimeout, margin);
    impersonationData.left = new FormAttachment(middle, 0);
    impersonationData.right = new FormAttachment(wbPrivateKey, -margin);
    wImpersonation.setLayoutData(impersonationData);

    // test service - Button
    Button wbTestServiceAccount = new Button(serviceAccountComposite, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTestServiceAccount);
    wbTestServiceAccount.setText(
        BaseMessages.getString(PKG, "GoogleSheetsDialog.Button.TestConnection"));
    FormData fdbTestServiceAccount = new FormData();
    fdbTestServiceAccount.top = new FormAttachment(wImpersonation, margin);
    fdbTestServiceAccount.left = new FormAttachment(0, 0);
    wbTestServiceAccount.setLayoutData(fdbTestServiceAccount);
    wbTestServiceAccount.addListener(SWT.Selection, e -> testServiceAccount());

    wlTestServiceAccountInfo = new Label(serviceAccountComposite, SWT.LEFT);
    props.setLook(wlTestServiceAccountInfo);
    FormData fdTestServiceAccountInfo = new FormData();
    fdTestServiceAccountInfo.top = new FormAttachment(wImpersonation, margin);
    fdTestServiceAccountInfo.left = new FormAttachment(middle, 0);
    fdTestServiceAccountInfo.right = new FormAttachment(100, 0);
    wlTestServiceAccountInfo.setLayoutData(fdTestServiceAccountInfo);

    FormData fdServiceAccountComposite = new FormData();
    fdServiceAccountComposite.left = new FormAttachment(0, 0);
    fdServiceAccountComposite.top = new FormAttachment(0, 0);
    fdServiceAccountComposite.right = new FormAttachment(100, 0);
    fdServiceAccountComposite.bottom = new FormAttachment(100, 0);
    serviceAccountComposite.setLayoutData(fdServiceAccountComposite);

    serviceAccountComposite.layout();
    serviceAccountTab.setControl(serviceAccountComposite);
    /*
     * END Service Account Tab
     */

    /*
     * BEGIN Spreadsheet Tab
     */
    CTabItem spreadsheetTab = new CTabItem(tabFolder, SWT.NONE);
    spreadsheetTab.setText(BaseMessages.getString(PKG, "GoogleSheetsDialog.Tab.Spreadsheet"));

    Composite spreadsheetComposite = new Composite(tabFolder, SWT.NONE);
    props.setLook(spreadsheetComposite);

    FormLayout spreadsheetLayout = new FormLayout();
    spreadsheetLayout.marginWidth = 3;
    spreadsheetLayout.marginHeight = 3;
    spreadsheetComposite.setLayout(spreadsheetLayout);

    // spreadsheetKey - Label
    Label wlSpreadsheetKey = new Label(spreadsheetComposite, SWT.RIGHT);
    wlSpreadsheetKey.setText(BaseMessages.getString(PKG, "GoogleSheetsDialog.SpreadsheetKey"));
    props.setLook(wlSpreadsheetKey);
    FormData fdlSpreadsheetKey = new FormData();
    fdlSpreadsheetKey.top = new FormAttachment(0, margin);
    fdlSpreadsheetKey.left = new FormAttachment(0, 0);
    fdlSpreadsheetKey.right = new FormAttachment(middle, -margin);
    wlSpreadsheetKey.setLayoutData(fdlSpreadsheetKey);

    // spreadsheetKey - Button
    Button wbSpreadsheetKey = new Button(spreadsheetComposite, SWT.PUSH | SWT.CENTER);
    wbSpreadsheetKey.setText(BaseMessages.getString("System.Button.Browse"));
    props.setLook(wbSpreadsheetKey);
    FormData fdbSpreadsheetKey = new FormData();
    fdbSpreadsheetKey.top = new FormAttachment(0, margin);
    fdbSpreadsheetKey.right = new FormAttachment(100, 0);
    wbSpreadsheetKey.setLayoutData(fdbSpreadsheetKey);
    wbSpreadsheetKey.addListener(SWT.Selection, e -> selectSpreadSheetKey());

    // spreadsheetKey - Text
    wSpreadsheetKey =
        new TextVar(variables, spreadsheetComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSpreadsheetKey);
    FormData fdSpreadsheetKey = new FormData();
    fdSpreadsheetKey.top = new FormAttachment(0, margin);
    fdSpreadsheetKey.left = new FormAttachment(middle, 0);
    fdSpreadsheetKey.right = new FormAttachment(wbSpreadsheetKey, -margin);
    wSpreadsheetKey.setLayoutData(fdSpreadsheetKey);

    // worksheetId - Label
    Label wlWorksheetId = new Label(spreadsheetComposite, SWT.RIGHT);
    wlWorksheetId.setText(BaseMessages.getString(PKG, "GoogleSheetsDialog.WorksheetId"));
    props.setLook(wlWorksheetId);
    FormData fdlWorksheetId = new FormData();
    fdlWorksheetId.top = new FormAttachment(wbSpreadsheetKey, margin);
    fdlWorksheetId.left = new FormAttachment(0, 0);
    fdlWorksheetId.right = new FormAttachment(middle, -margin);
    wlWorksheetId.setLayoutData(fdlWorksheetId);

    // worksheetId - Button
    Button wbWorksheetId = new Button(spreadsheetComposite, SWT.PUSH | SWT.CENTER);
    wbWorksheetId.setText(BaseMessages.getString("System.Button.Browse"));
    props.setLook(wbWorksheetId);
    FormData fdbWorksheetId = new FormData();
    fdbWorksheetId.top = new FormAttachment(wbSpreadsheetKey, margin);
    fdbWorksheetId.right = new FormAttachment(100, 0);
    wbWorksheetId.setLayoutData(fdbWorksheetId);
    wbWorksheetId.addListener(SWT.Selection, e -> selectWorksheet());

    // worksheetId - Text
    wWorksheetId = new TextVar(variables, spreadsheetComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wWorksheetId);
    FormData fdWorksheetId = new FormData();
    fdWorksheetId.top = new FormAttachment(wbSpreadsheetKey, margin);
    fdWorksheetId.left = new FormAttachment(middle, 0);
    fdWorksheetId.right = new FormAttachment(wbWorksheetId, -margin);
    wWorksheetId.setLayoutData(fdWorksheetId);

    // Append tick box label
    Label wlAppend = new Label(spreadsheetComposite, SWT.RIGHT);
    wlAppend.setText(BaseMessages.getString(PKG, "GoogleSheetsOutputDialog.Append.Label"));
    props.setLook(wlAppend);
    FormData fdlAppend = new FormData();
    fdlAppend.top = new FormAttachment(wbWorksheetId, margin);
    fdlAppend.left = new FormAttachment(0, 0);
    fdlAppend.right = new FormAttachment(middle, -margin);
    wlAppend.setLayoutData(fdlAppend);

    // Append tick box button
    wbAppend = new Button(spreadsheetComposite, SWT.CHECK);
    props.setLook(wbAppend);
    FormData fdbAppend = new FormData();
    fdbAppend.top = new FormAttachment(wlAppend, 0, SWT.CENTER);
    fdbAppend.left = new FormAttachment(middle, 0);
    fdbAppend.right = new FormAttachment(100, 0);
    wbAppend.setLayoutData(fdbAppend);

    // Create New Sheet tick box label
    Label wlCreate = new Label(spreadsheetComposite, SWT.RIGHT);
    wlCreate.setText(BaseMessages.getString(PKG, "GoogleSheetsOutputDialog.Create.Label"));
    props.setLook(wlCreate);
    FormData fdlCreate = new FormData();
    fdlCreate.top = new FormAttachment(wlAppend, 2 * margin);
    fdlCreate.left = new FormAttachment(0, 0);
    fdlCreate.right = new FormAttachment(middle, -margin);
    wlCreate.setLayoutData(fdlCreate);

    // Create New Sheet tick box button
    wbCreate = new Button(spreadsheetComposite, SWT.CHECK);
    props.setLook(wbCreate);
    FormData fdbCreate = new FormData();
    fdbCreate.top = new FormAttachment(wlCreate, 0, SWT.CENTER);
    fdbCreate.left = new FormAttachment(middle, 0);
    fdbCreate.right = new FormAttachment(100, 0);
    wbCreate.setLayoutData(fdbCreate);

    // Share spreadsheet with label
    Label wlShare = new Label(spreadsheetComposite, SWT.RIGHT);
    wlShare.setText(BaseMessages.getString(PKG, "GoogleSheetsOutputDialog.Share.Label"));
    props.setLook(wlShare);
    FormData fdlShare = new FormData();
    fdlShare.top = new FormAttachment(wlCreate, 2 * margin);
    fdlShare.left = new FormAttachment(0, 0);
    fdlShare.right = new FormAttachment(middle, -margin);
    wlShare.setLayoutData(fdlShare);
    // Share spreadsheet with label
    wShareEmail = new TextVar(variables, spreadsheetComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wShareEmail);
    FormData fdShare = new FormData();
    fdShare.top = new FormAttachment(wlShare, 0, SWT.CENTER);
    fdShare.left = new FormAttachment(middle, 0);
    fdShare.right = new FormAttachment(100, 0);
    wShareEmail.setLayoutData(fdShare);

    // Share domainwise with label
    Label wlShareDomainWise = new Label(spreadsheetComposite, SWT.RIGHT);
    wlShareDomainWise.setText(
        BaseMessages.getString(PKG, "GoogleSheetsOutputDialog.Share.LabelDW"));
    props.setLook(wlShareDomainWise);
    FormData fdlShareDomainWise = new FormData();
    fdlShareDomainWise.top = new FormAttachment(wShareEmail, margin);
    fdlShareDomainWise.left = new FormAttachment(0, 0);
    fdlShareDomainWise.right = new FormAttachment(middle, -margin);
    wlShareDomainWise.setLayoutData(fdlShareDomainWise);
    // Share domainwise with label
    wShareDomainWise =
        new TextVar(variables, spreadsheetComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wShareDomainWise);
    FormData fdShareDomainWise = new FormData();
    fdShareDomainWise.top = new FormAttachment(wShareEmail, margin);
    fdShareDomainWise.left = new FormAttachment(middle, 0);
    fdShareDomainWise.right = new FormAttachment(100, 0);
    wShareDomainWise.setLayoutData(fdShareDomainWise);

    FormData spreadsheetCompositeData = new FormData();
    spreadsheetCompositeData.left = new FormAttachment(0, 0);
    spreadsheetCompositeData.top = new FormAttachment(0, 0);
    spreadsheetCompositeData.right = new FormAttachment(100, 0);
    spreadsheetCompositeData.bottom = new FormAttachment(100, 0);
    spreadsheetComposite.setLayoutData(spreadsheetCompositeData);

    spreadsheetComposite.layout();
    spreadsheetTab.setControl(spreadsheetComposite);
    /*
     * END Spreadsheet Tab
     */

    FormData tabFolderData = new FormData();
    tabFolderData.left = new FormAttachment(0, 0);
    tabFolderData.top = new FormAttachment(wTransformName, margin);
    tabFolderData.right = new FormAttachment(100, 0);
    tabFolderData.bottom = new FormAttachment(wOk, -2 * margin);
    tabFolder.setLayoutData(tabFolderData);

    tabFolder.setSelection(0);

    getData(meta);
    meta.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void selectWorksheet() {
    try {

      NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
      String scope = SheetsScopes.SPREADSHEETS_READONLY;

      HttpRequestInitializer credential =
          GoogleSheetsCredentials.getCredentialsJson(
              scope,
              variables.resolve(meta.getJsonCredentialPath()),
              variables.resolve(meta.getImpersonation()));
      Sheets service =
          new Sheets.Builder(
                  HTTP_TRANSPORT,
                  JSON_FACTORY,
                  GoogleSheetsCredentials.setHttpTimeout(
                      credential, variables.resolve(meta.getTimeout())))
              .setApplicationName(GoogleSheetsCredentials.APPLICATION_NAME)
              .build();
      Spreadsheet response1 =
          service.spreadsheets().get(wSpreadsheetKey.getText()).setIncludeGridData(false).execute();

      List<Sheet> worksheets = response1.getSheets();
      String[] names = new String[worksheets.size()];
      int selectedSheet = -1;
      for (int i = 0; i < worksheets.size(); i++) {
        Sheet sheet = worksheets.get(i);
        names[i] = sheet.getProperties().getTitle();
        if (sheet.getProperties().getTitle().endsWith("/" + wWorksheetId.getText())) {
          selectedSheet = i;
        }
      }

      EnterSelectionDialog esd =
          new EnterSelectionDialog(shell, names, "Worksheets", "Select a Worksheet.");
      if (selectedSheet > -1) {
        esd.setSelectedNrs(new int[] {selectedSheet});
      }
      String s = esd.open();
      if (s != null) {
        if (esd.getSelectionIndeces().length > 0) {
          selectedSheet = esd.getSelectionIndeces()[0];
          Sheet sheet = worksheets.get(selectedSheet);
          String id = sheet.getProperties().getTitle();
          wWorksheetId.setText(id.substring(id.lastIndexOf("/") + 1));
        } else {
          wWorksheetId.setText("");
        }
      }

    } catch (Exception err) {
      new ErrorDialog(
          shell, BaseMessages.getString(PKG, "System.Dialog.Error.Title"), err.getMessage(), err);
    }
  }

  private void selectSpreadSheetKey() {
    try {
      NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
      String scope = "https://www.googleapis.com/auth/drive";
      HttpRequestInitializer credential =
          GoogleSheetsCredentials.getCredentialsJson(
              scope,
              variables.resolve(meta.getJsonCredentialPath()),
              variables.resolve(meta.getImpersonation()));
      Drive service =
          new Drive.Builder(
                  HTTP_TRANSPORT,
                  JSON_FACTORY,
                  GoogleSheetsCredentials.setHttpTimeout(
                      credential, variables.resolve(meta.getTimeout())))
              .setApplicationName(GoogleSheetsCredentials.APPLICATION_NAME)
              .build();

      FileList result =
          service
              .files()
              .list()
              .setSupportsAllDrives(true)
              .setIncludeItemsFromAllDrives(true)
              .setQ("mimeType='application/vnd.google-apps.spreadsheet'")
              .setPageSize(100)
              .setFields("nextPageToken, files(id, name)")
              .execute();
      List<File> spreadsheets = result.getFiles();
      int selectedSpreadsheet = -1;
      int i = 0;
      String[] titles = new String[spreadsheets.size()];
      for (File spreadsheet : spreadsheets) {
        titles[i] = spreadsheet.getName() + " - " + spreadsheet.getId();
        if (spreadsheet.getId().equals(wSpreadsheetKey.getText())) {
          selectedSpreadsheet = i;
        }
        i++;
      }

      EnterSelectionDialog esd =
          new EnterSelectionDialog(shell, titles, "Spreadsheets", "Select a Spreadsheet.");
      if (selectedSpreadsheet > -1) {
        esd.setSelectedNrs(new int[] {selectedSpreadsheet});
      }
      String s = esd.open();
      if (s != null) {
        if (esd.getSelectionIndeces().length > 0) {
          selectedSpreadsheet = esd.getSelectionIndeces()[0];
          File spreadsheet = spreadsheets.get(selectedSpreadsheet);
          wSpreadsheetKey.setText(spreadsheet.getId());
        } else {
          wSpreadsheetKey.setText("");
        }
      }

    } catch (Exception err) {
      new ErrorDialog(shell, "System.Dialog.Error.Title", err.getMessage(), err);
    }
  }

  private void testServiceAccount() {
    try {
      NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
      String scope = SheetsScopes.SPREADSHEETS_READONLY;

      HttpRequestInitializer credential =
          GoogleSheetsCredentials.getCredentialsJson(
              scope,
              variables.resolve(meta.getJsonCredentialPath()),
              variables.resolve(meta.getImpersonation()));
      // Build a Drive connection to test it
      //
      new Drive.Builder(
              HTTP_TRANSPORT,
              JSON_FACTORY,
              GoogleSheetsCredentials.setHttpTimeout(
                  credential, variables.resolve(meta.getTimeout())))
          .setApplicationName(GoogleSheetsCredentials.APPLICATION_NAME)
          .build();
      wlTestServiceAccountInfo.setText("Google Drive API : Success!");
    } catch (Exception error) {
      wlTestServiceAccountInfo.setText("Connection Failed");
    }
  }

  private void selectPrivateKeyFile() {
    String filename =
        BaseDialog.presentFileDialog(
            shell,
            new String[] {"*json", "*"},
            new String[] {"credential JSON file", "All Files"},
            true);
    if (filename != null) {
      wPrivateKeyStore.setText(filename);
      meta.setChanged();
    }
  }

  private void getData(GoogleSheetsOutputMeta meta) {
    this.wTransformName.selectAll();

    if (!StringUtils.isEmpty(meta.getSpreadsheetKey())) {
      this.wSpreadsheetKey.setText(meta.getSpreadsheetKey());
    }
    if (!StringUtils.isEmpty(meta.getWorksheetId())) {
      this.wWorksheetId.setText(meta.getWorksheetId());
    }
    if (!StringUtils.isEmpty(meta.getShareEmail())) {
      this.wShareEmail.setText(meta.getShareEmail());
    }
    if (!StringUtils.isEmpty(meta.getTimeout())) {
      this.wTimeout.setText(meta.getTimeout());
    }
    if (!StringUtils.isEmpty(meta.getImpersonation())) {
      this.wImpersonation.setText(meta.getImpersonation());
    }
    if (!StringUtils.isEmpty(meta.getAppName())) {
      this.wAppName.setText(meta.getAppName());
    }
    if (!StringUtils.isEmpty(meta.getShareDomain())) {
      this.wShareDomainWise.setText(meta.getShareDomain());
    }
    if (!StringUtils.isEmpty(meta.getJsonCredentialPath())) {
      this.wPrivateKeyStore.setText(meta.getJsonCredentialPath());
    }

    this.wbCreate.setSelection(meta.isCreate());
    this.wbAppend.setSelection(meta.isAppend());
  }

  private void setData(GoogleSheetsOutputMeta meta) {

    meta.setJsonCredentialPath(this.wPrivateKeyStore.getText());
    meta.setSpreadsheetKey(this.wSpreadsheetKey.getText());
    meta.setWorksheetId(this.wWorksheetId.getText());
    meta.setShareEmail(this.wShareEmail.getText());
    meta.setCreate(this.wbCreate.getSelection());
    meta.setAppend(this.wbAppend.getSelection());
    meta.setShareDomain(this.wShareDomainWise.getText());

    meta.setTimeout(this.wTimeout.getText());
    meta.setAppName(this.wAppName.getText());
    meta.setImpersonation(this.wImpersonation.getText());
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  private void ok() {
    transformName = wTransformName.getText();
    setData(this.meta);
    meta.setChanged();
    dispose();
  }
}
